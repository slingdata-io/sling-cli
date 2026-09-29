package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

func init() {
	Register("bws", newBWSProvider, "bitwarden_secrets_manager")
	Register("bw", newBWProvider, "bitwarden")
}

// bwsProvider reads Bitwarden Secrets Manager secrets with the bws CLI.
// `ref+bws://<secret-uuid>`.
type bwsProvider struct {
	runner    *cliRunner
	serverURL string
}

func newBWSProvider(cfg ProviderConfig) (Provider, error) {
	runner := newCLIRunner(cfg, "bws", "Install the Bitwarden Secrets Manager CLI (bws).")
	runner.setEnv("BWS_ACCESS_TOKEN", cfg.Get("token"))
	return &bwsProvider{runner: runner, serverURL: cfg.Get("server_url", "BWS_SERVER_URL")}, nil
}

func (p *bwsProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	args := []string{"secret", "get", ref.Path, "--output", "json"}
	if p.serverURL != "" {
		args = append(args, "--server-url", p.serverURL)
	}
	out, err := p.runner.Run(ctx, nil, args...)
	if err != nil {
		return nil, err
	}
	var secret struct {
		Value *string `json:"value"`
	}
	if err := json.Unmarshal(out, &secret); err != nil || secret.Value == nil {
		return nil, fmt.Errorf("bws output has no value")
	}
	return []byte(*secret.Value), nil
}

// bwProvider reads Bitwarden Password Manager items with the bw CLI.
// `ref+bw://<item-id>/<field>`. The vault must be unlocked (BW_SESSION).
type bwProvider struct {
	runner *cliRunner
}

func newBWProvider(cfg ProviderConfig) (Provider, error) {
	runner := newCLIRunner(cfg, "bw", "Install the Bitwarden CLI (bw), and unlock the vault (BW_SESSION).")
	runner.setEnv("BW_SESSION", cfg.Get("session"))
	return &bwProvider{runner: runner}, nil
}

func (p *bwProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	id, field, _ := strings.Cut(ref.Path, "/")
	out, err := p.runner.Run(ctx, nil, "get", "item", id)
	if err != nil {
		return nil, err
	}
	if field == "" {
		return out, nil
	}
	var item struct {
		Notes *string `json:"notes"`
		Login *struct {
			Username *string `json:"username"`
			Password *string `json:"password"`
			Totp     *string `json:"totp"`
		} `json:"login"`
		Fields []struct {
			Name  string  `json:"name"`
			Value *string `json:"value"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(out, &item); err != nil {
		return nil, fmt.Errorf("bw output is not an item")
	}
	var val *string
	switch strings.ToLower(field) {
	case "notes":
		val = item.Notes
	case "username", "password", "totp":
		if item.Login != nil {
			val = map[string]*string{
				"username": item.Login.Username,
				"password": item.Login.Password,
				"totp":     item.Login.Totp,
			}[strings.ToLower(field)]
		}
	default:
		for _, f := range item.Fields {
			if f.Name == field {
				val = f.Value
				break
			}
		}
	}
	if val == nil {
		return nil, fmt.Errorf("field %q not found in item %q", field, id)
	}
	return []byte(*val), nil
}
