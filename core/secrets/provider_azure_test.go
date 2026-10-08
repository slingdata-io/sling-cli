package secrets

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

type fakeAzureCredential struct{}

func (fakeAzureCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "fake-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// azureTestServer is a fake Key Vault. It returns the vault URL and the
// path of the last secret request.
func azureTestServer(t *testing.T) (string, *string) {
	gotPath := new(string)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// the client first sends a request with no token, to get the challenge
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Bearer authorization="https://login.microsoftonline.com/tenant", resource="https://vault.azure.net"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		*gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"value":"kv-pass-789","id":"https://acme-kv.vault.azure.net/secrets/pg-password/v1"}`))
	}))
	t.Cleanup(srv.Close)

	oldCred, oldOpts := newAzureCredential, azureClientOptions
	t.Cleanup(func() { newAzureCredential, azureClientOptions = oldCred, oldOpts })
	newAzureCredential = func(ProviderConfig) (azcore.TokenCredential, error) { return fakeAzureCredential{}, nil }
	azureClientOptions = &azsecrets.ClientOptions{
		ClientOptions:                        azcore.ClientOptions{Transport: srv.Client()},
		DisableChallengeResourceVerification: true,
	}
	return srv.URL, gotPath
}

func TestAzureKeyVault(t *testing.T) {
	vaultURL, gotPath := azureTestServer(t)
	cfg, err := ParseConfig(map[string]map[string]any{
		"kv": {"type": "azure_key_vault", "vault_url": vaultURL},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := NewResolver(Options{Config: cfg})
	v, err := r.Resolve(context.Background(), "ref+azurekeyvault://acme-kv/pg-password/v1")
	if err != nil {
		t.Fatal(err)
	}
	if v != "kv-pass-789" {
		t.Fatalf("got %v", v)
	}
	if !strings.HasPrefix(*gotPath, "/secrets/pg-password/v1") {
		t.Fatalf("path %q", *gotPath)
	}
}

func TestAzureKeyVaultSecretID(t *testing.T) {
	vaultURL, gotPath := azureTestServer(t)
	r := NewResolver(Options{})
	for _, tc := range []struct{ ref, path string }{
		{"ref+azurekeyvault://" + vaultURL + "/secrets/pg-password/v1", "/secrets/pg-password/v1"},
		{"ref+azurekeyvault://" + vaultURL + "/secrets/pg-password", "/secrets/pg-password"},
		{"ref+azurekeyvault://" + strings.TrimPrefix(vaultURL, "https://") + "/pg-password/v2", "/secrets/pg-password/v2"},
	} {
		v, err := r.Resolve(context.Background(), tc.ref)
		if err != nil {
			t.Fatal(err)
		}
		if v != "kv-pass-789" {
			t.Fatalf("got %v", v)
		}
		if !strings.HasPrefix(*gotPath, tc.path) {
			t.Fatalf("path %q, want %q", *gotPath, tc.path)
		}
	}

	_, err := r.Resolve(context.Background(), "ref+azurekeyvault://https://hsm1.managedhsm.azure.net/secrets/x")
	if err == nil || !strings.Contains(err.Error(), "managed HSM") {
		t.Fatalf("error %v", err)
	}

	_, err = r.Resolve(context.Background(), "ref+azurekeyvault://"+vaultURL+"/keys/k1")
	if err == nil || !strings.Contains(err.Error(), "/secrets/<name>") {
		t.Fatalf("error %v", err)
	}
}

func TestAzureKeyVaultBadPath(t *testing.T) {
	r := NewResolver(Options{})
	_, err := r.Resolve(context.Background(), "ref+azurekeyvault://only-vault")
	if err == nil || !strings.Contains(err.Error(), "<vault>/<name>") {
		t.Fatalf("error %v", err)
	}
	_, err = r.Resolve(context.Background(), "ref+azurekeyvault://v/n?cloud=mars")
	if err == nil || !strings.Contains(err.Error(), "unknown cloud") {
		t.Fatalf("error %v", err)
	}
}
