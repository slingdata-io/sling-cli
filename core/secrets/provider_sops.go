package secrets

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
)

func init() { Register("sops", newSOPSProvider) }

// sopsProvider decrypts SOPS files with the sops CLI. It decrypts each file
// once and keeps the result. The resolver applies the pointer.
// `ref+sops://secrets.enc.yaml#/pg/password`.
type sopsProvider struct {
	runner  *cliRunner
	baseDir string

	mu    sync.Mutex
	files map[string][]byte
}

func newSOPSProvider(cfg ProviderConfig) (Provider, error) {
	return &sopsProvider{
		runner:  newCLIRunner(cfg, "sops", "Install sops (https://github.com/getsops/sops)."),
		baseDir: cfg.BaseDir(),
		files:   map[string][]byte{},
	}, nil
}

func (p *sopsProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	path := ref.Path
	if !filepath.IsAbs(path) && p.baseDir != "" {
		path = filepath.Join(p.baseDir, path)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if doc, ok := p.files[path]; ok {
		return doc, nil
	}

	args := []string{"-d"}
	format := ""
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		format = "json"
	case ".env":
		format = "dotenv"
	}
	if format != "" {
		args = append(args, "--input-type", format, "--output-type", format)
	}
	out, err := p.runner.Run(ctx, nil, append(args, path)...)
	if err != nil {
		return nil, err
	}
	if format == "dotenv" {
		if out, err = dotenvToJSON(out); err != nil {
			return nil, err
		}
	}
	p.files[path] = out
	return out, nil
}

// GetMany decrypts each file once.
func (p *sopsProvider) GetMany(ctx context.Context, refs []Ref) (map[string][]byte, error) {
	out := map[string][]byte{}
	for _, ref := range refs {
		doc, err := p.Get(ctx, ref)
		if err != nil {
			return nil, err
		}
		out[ref.Key()] = doc
	}
	return out, nil
}

// dotenvToJSON turns KEY=VALUE lines into a JSON object, so pointers apply.
func dotenvToJSON(b []byte) ([]byte, error) {
	obj := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		obj[strings.TrimSpace(k)] = v
	}
	return json.Marshal(obj)
}
