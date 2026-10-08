// Package secrets resolves secret references (`op://...`, `ref+<backend>://...`)
// against external secret managers.
package secrets

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// Provider fetches the raw value of one secret. The resolver applies the
// pointer, trim, cache, and redaction. A provider does none of these.
type Provider interface {
	Get(ctx context.Context, ref Ref) ([]byte, error)
}

// Factory builds a provider instance from its config.
type Factory func(cfg ProviderConfig) (Provider, error)

var registry = struct {
	sync.RWMutex
	factories map[string]Factory
	aliases   map[string]string
}{factories: map[string]Factory{}, aliases: map[string]string{}}

// Register adds a backend kind. Aliases map other scheme names to it.
func Register(kind string, f Factory, aliases ...string) {
	registry.Lock()
	defer registry.Unlock()
	kind = strings.ToLower(kind)
	registry.factories[kind] = f
	for _, alias := range aliases {
		registry.aliases[strings.ToLower(alias)] = kind
	}
}

// Kinds returns the registered kinds, sorted.
func Kinds() []string {
	registry.RLock()
	defer registry.RUnlock()
	kinds := make([]string, 0, len(registry.factories))
	for k := range registry.factories {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	return kinds
}

// lookupKind maps a kind or alias to its registered kind.
func lookupKind(name string) (string, bool) {
	registry.RLock()
	defer registry.RUnlock()
	name = strings.ToLower(strings.TrimSpace(name))
	if _, ok := registry.factories[name]; ok {
		return name, true
	}
	kind, ok := registry.aliases[name]
	return kind, ok
}

func factoryOf(kind string) Factory {
	registry.RLock()
	defer registry.RUnlock()
	return registry.factories[kind]
}
