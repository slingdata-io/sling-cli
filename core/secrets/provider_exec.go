package secrets

import "context"

func init() { Register("exec", newExecProvider) }

// execProvider runs a local command and reads its stdout.
// `ref+exec://my-tool?arg=get&arg=pg` runs `my-tool get pg`, with no shell.
type execProvider struct{ cfg ProviderConfig }

func newExecProvider(cfg ProviderConfig) (Provider, error) {
	return &execProvider{cfg: cfg}, nil
}

func (p *execProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	runner := newCLIRunner(ProviderConfig{redact: p.cfg.redact}, ref.Path, "")
	return runner.Run(ctx, nil, ref.Params["arg"]...)
}
