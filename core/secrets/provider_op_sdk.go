//go:build secrets_opsdk

package secrets

import (
	"context"
	"errors"
	"fmt"
	"sync"

	onepassword "github.com/1password/onepassword-sdk-go"
)

func init() { opSDKFactory = newOPSDK }

// opSDK reads 1Password secrets with the 1Password Go SDK.
// The client is built at the first fetch.
type opSDK struct {
	token string

	once   sync.Once
	client *onepassword.Client
	err    error
}

func newOPSDK(cfg ProviderConfig) (Provider, error) {
	token := cfg.Get("token", "OP_SERVICE_ACCOUNT_TOKEN")
	if token == "" {
		return nil, errors.New("the 1Password SDK needs token or OP_SERVICE_ACCOUNT_TOKEN")
	}
	return &opSDK{token: token}, nil
}

func (s *opSDK) connect(ctx context.Context) (*onepassword.Client, error) {
	s.once.Do(func() {
		s.client, s.err = onepassword.NewClient(ctx,
			onepassword.WithServiceAccountToken(s.token),
			onepassword.WithIntegrationInfo("Sling", "v1"),
		)
	})
	return s.client, s.err
}

func (s *opSDK) Get(ctx context.Context, ref Ref) ([]byte, error) {
	client, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	v, err := client.Secrets().Resolve(ctx, opURI(ref))
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

func (s *opSDK) GetMany(ctx context.Context, refs []Ref) (map[string][]byte, error) {
	client, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	uris := make([]string, len(refs))
	for i, ref := range refs {
		uris[i] = opURI(ref)
	}
	resp, err := client.Secrets().ResolveAll(ctx, uris)
	if err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for i, ref := range refs {
		r, ok := resp.IndividualResponses[uris[i]]
		if !ok || r.Content == nil {
			return nil, fmt.Errorf("1Password SDK returned no value for %s", ref.Raw)
		}
		out[ref.Key()] = []byte(r.Content.Secret)
	}
	return out, nil
}
