package secrets

import (
	"context"
	"errors"
	"sync"

	ksm "github.com/keeper-security/secrets-manager-go/core"
)

func init() { Register("keeper", newKeeperProvider) }

// keeperProvider reads Keeper Secrets Manager values with Keeper notation:
// `keeper://<UID or title>/field/password`. The client is built at the first
// fetch.
type keeperProvider struct {
	config string // base64 or JSON KSM config

	once sync.Once
	sm   *ksm.SecretsManager
	err  error
	mu   sync.Mutex // the SDK client is not safe for concurrent use
}

func newKeeperProvider(cfg ProviderConfig) (Provider, error) {
	return &keeperProvider{config: cfg.Get("config", "KSM_CONFIG")}, nil
}

// keeperNotation is the Keeper notation for ref.
func keeperNotation(ref Ref) string { return "keeper://" + ref.Path }

func (p *keeperProvider) client() (*ksm.SecretsManager, error) {
	p.once.Do(func() {
		if p.config == "" {
			p.err = errors.New("Keeper needs config or KSM_CONFIG (the base64 KSM config)")
			return
		}
		p.sm = ksm.NewSecretsManager(&ksm.ClientOptions{Config: ksm.NewMemoryKeyValueStorage(p.config)})
		if p.sm == nil {
			p.err = errors.New("the Keeper config is not valid")
		}
	})
	return p.sm, p.err
}

func (p *keeperProvider) Get(_ context.Context, ref Ref) ([]byte, error) {
	sm, err := p.client()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	vals, err := sm.GetNotationResults(keeperNotation(ref))
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, errors.New("the Keeper notation has no value")
	}
	return []byte(vals[0]), nil
}
