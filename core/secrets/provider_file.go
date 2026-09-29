package secrets

import (
	"context"
	"os"
	"path/filepath"
	"strings"
)

func init() { Register("file", newFileProvider) }

// fileProvider reads a local file. A relative path is from the env.yaml folder.
type fileProvider struct{ baseDir string }

func newFileProvider(cfg ProviderConfig) (Provider, error) {
	return &fileProvider{baseDir: cfg.BaseDir()}, nil
}

func (p *fileProvider) Get(_ context.Context, ref Ref) ([]byte, error) {
	path := ref.Path
	if rest, ok := strings.CutPrefix(path, "~/"); ok {
		if home, err := os.UserHomeDir(); err == nil {
			path = filepath.Join(home, rest)
		}
	}
	if !filepath.IsAbs(path) && p.baseDir != "" {
		path = filepath.Join(p.baseDir, path)
	}
	return os.ReadFile(path)
}
