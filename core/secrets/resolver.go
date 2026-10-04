package secrets

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// batchGetter fetches many refs in one call (op inject, Doppler download, sops file).
// The result is keyed by Ref.Key.
type batchGetter interface {
	GetMany(ctx context.Context, refs []Ref) (map[string][]byte, error)
}

// closer releases clients (SDK sessions, temp files).
type closer interface{ Close() error }

// maxConcurrentFetches limits parallel provider calls in ResolveValue.
const maxConcurrentFetches = 8

// CredentialKeys are provider config keys that hold credentials. Their values
// are redacted like resolved values.
var CredentialKeys = []string{
	"token", "connect_token", "secret_id", "client_secret", "api_key", "access_key",
	"password", "jwt", "credentials_json", "config", "session",
}

// minRedactLen skips short values, so "on" or "5432" do not redact log text.
const minRedactLen = 4

type Options struct {
	Config   Config
	CacheTTL time.Duration  // 0 keeps values for the process life
	Timeout  time.Duration  // per fetch, default 30s
	OnValue  func(v string) // called for each resolved value (redaction hook)
	BaseDir  string         // folder for relative `file` paths

	// IsSecret tells if the value at a key path is redacted. The path is
	// empty when the key is not known. Nil redacts all values.
	IsSecret func(path []string) bool

	// KeyAliases renames keys of a `from:` object, e.g. username -> user.
	KeyAliases map[string]string
}

type cacheEntry struct {
	raw     []byte
	expires time.Time // zero = no expiry
}

// Resolver turns secret references into values. It is safe for concurrent use.
type Resolver struct {
	opts Options

	imu       sync.Mutex // guards instances
	instances map[string]Provider

	mu     sync.Mutex // guards cache and values
	cache  map[string]cacheEntry
	values map[string]struct{} // resolved values, for Redact
	sf     singleflight.Group
}

func NewResolver(opts Options) *Resolver {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.Config == nil {
		opts.Config = Config{}
	}
	return &Resolver{
		opts:      opts,
		instances: map[string]Provider{},
		cache:     map[string]cacheEntry{},
		values:    map[string]struct{}{},
	}
}

// Resolve returns the value for one reference string. A string that is not a
// reference comes back unchanged. A pointer to an object is an error: objects
// are valid only in `from:`.
// Its value is always redacted.
func (r *Resolver) Resolve(ctx context.Context, s string) (any, error) {
	return r.resolve(ctx, s, nil)
}

// resolve is Resolve for the value at path.
func (r *Resolver) resolve(ctx context.Context, s string, path []string) (any, error) {
	if !IsRef(s) {
		if embeddedRefRe.MatchString(s) {
			return r.resolveEmbedded(ctx, s, path)
		}
		return s, nil
	}
	ref, err := ParseRef(s)
	if err != nil {
		return nil, err
	}
	v, err := r.value(ctx, ref)
	if err != nil {
		return nil, err
	}
	switch v.(type) {
	case map[string]any, []any:
		return nil, r.wrap(ref, fmt.Errorf("the value is an object or a list. Use it in `from:`, or add a pointer such as #/password"))
	}
	r.rememberAt(path, v)
	return v, nil
}

// ResolveValue deep-walks maps and lists and replaces every reference.
// It returns a copy. It fetches unique Ref.Key values concurrently.
func (r *Resolver) ResolveValue(ctx context.Context, v any) (any, error) {
	var refs []Ref
	var parseErr error
	walkStrings(v, func(s string) {
		for _, raw := range refsIn(s) {
			ref, err := ParseRef(raw)
			if err != nil && parseErr == nil {
				parseErr = err
			}
			refs = append(refs, ref)
		}
	})
	if parseErr != nil {
		return nil, parseErr
	}
	if err := r.prefetch(ctx, refs); err != nil {
		return nil, err
	}
	return r.replace(ctx, v, nil)
}

// ResolveEntry is ResolveValue plus the `from:` merge for one connection entry.
// Keys written in the entry win over keys from the secret.
func (r *Resolver) ResolveEntry(ctx context.Context, props map[string]any) (map[string]any, error) {
	rest := make(map[string]any, len(props))
	var from string
	for k, v := range props {
		if strings.EqualFold(k, "from") {
			from = strings.TrimSpace(fmt.Sprint(v))
			continue
		}
		rest[k] = v
	}

	resolved, err := r.ResolveValue(ctx, rest)
	if err != nil {
		return nil, err
	}
	out := resolved.(map[string]any)
	if from == "" {
		return out, nil
	}

	obj, err := r.fromObject(ctx, from)
	if err != nil {
		return nil, err
	}
	for k, v := range obj {
		if _, set := out[k]; !set {
			out[k] = v
			r.rememberAt([]string{k}, v)
		}
	}
	return out, nil
}

// fromObject loads a `from:` reference as an object with lower-case keys.
func (r *Resolver) fromObject(ctx context.Context, from string) (map[string]any, error) {
	if !IsRef(from) {
		return nil, fmt.Errorf("`from` must be a secret reference such as ref+awssecrets://prod/postgres, got a literal value")
	}
	ref, err := ParseRef(from)
	if err != nil {
		return nil, err
	}
	v, err := r.value(ctx, ref)
	if err != nil {
		return nil, err
	}
	if s, ok := v.(string); ok {
		if doc, perr := parseDocument([]byte(s)); perr == nil {
			v = doc
		}
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, r.wrap(ref, fmt.Errorf("`from` needs a JSON or YAML object"))
	}
	obj := make(map[string]any, len(m))
	for k, val := range m {
		obj[strings.ToLower(k)] = val
	}
	for alias, key := range r.opts.KeyAliases {
		if val, ok := obj[alias]; ok {
			if _, set := obj[key]; !set {
				obj[key] = val
			}
			delete(obj, alias)
		}
	}
	return obj, nil
}

// Redact replaces every resolved value in s with ***.
func (r *Resolver) Redact(s string) string {
	r.mu.Lock()
	vals := make([]string, 0, len(r.values))
	for v := range r.values {
		vals = append(vals, v)
	}
	r.mu.Unlock()
	// longest first, so a value that contains another is replaced whole
	sort.Slice(vals, func(i, j int) bool { return len(vals[i]) > len(vals[j]) })
	for _, v := range vals {
		s = strings.ReplaceAll(s, v, "***")
	}
	return s
}

func (r *Resolver) Close() error {
	r.imu.Lock()
	defer r.imu.Unlock()
	var firstErr error
	for name, p := range r.instances {
		if c, ok := p.(closer); ok {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(r.instances, name)
	}
	return firstErr
}

// value fetches ref and applies trim and the pointer.
func (r *Resolver) value(ctx context.Context, ref Ref) (any, error) {
	raw, err := r.fetch(ctx, ref)
	if err != nil {
		return nil, err
	}
	if ref.Trim && ref.Pointer == "" {
		raw = trimNewline(raw)
	}
	v, err := ref.Select(raw)
	if err != nil {
		return nil, r.wrap(ref, err)
	}
	return v, nil
}

// fetch returns the raw secret, from the cache or the provider.
// fetchChainKey is the context key of the references in fetch, outer first.
type fetchChainKey struct{}

func (r *Resolver) fetch(ctx context.Context, ref Ref) ([]byte, error) {
	key := ref.Key()
	if raw, ok := r.cached(key); ok {
		return raw, nil
	}

	// a provider can resolve other references (sling): stop a cycle
	// here, before singleflight waits for itself
	chain, _ := ctx.Value(fetchChainKey{}).([]Ref)
	for _, prev := range chain {
		if prev.Key() == key {
			names := make([]string, 0, len(chain)+1)
			for _, c := range append(chain, ref) {
				names = append(names, c.Raw)
			}
			return nil, r.wrap(ref, fmt.Errorf("circular reference: %s", strings.Join(names, " -> ")))
		}
	}
	ctx = context.WithValue(ctx, fetchChainKey{}, append(slices.Clone(chain), ref))

	res, err, _ := r.sf.Do(key, func() (any, error) {
		if raw, ok := r.cached(key); ok {
			return raw, nil
		}
		p, err := r.instance(ref)
		if err != nil {
			return nil, err
		}
		fctx, cancel := context.WithTimeout(ctx, r.opts.Timeout)
		defer cancel()
		raw, err := p.Get(fctx, ref)
		if err != nil {
			return nil, err
		}
		r.store(key, raw)
		return raw, nil
	})
	if err != nil {
		return nil, r.wrap(ref, err)
	}
	return res.([]byte), nil
}

// prefetch loads refs into the cache: GetMany per instance when the provider
// has it, else single fetches with a concurrency limit.
func (r *Resolver) prefetch(ctx context.Context, refs []Ref) error {
	byKey := map[string]Ref{}
	for _, ref := range refs {
		if _, ok := r.cached(ref.Key()); !ok {
			byKey[ref.Key()] = ref
		}
	}
	if len(byKey) == 0 {
		return nil
	}

	groups := map[string][]Ref{}
	for _, ref := range byKey {
		name, _, err := r.choose(ref)
		if err != nil {
			return r.wrap(ref, err)
		}
		groups[name] = append(groups[name], ref)
	}

	var singles []Ref
	for _, group := range groups {
		p, err := r.instance(group[0])
		if err != nil {
			return r.wrap(group[0], err)
		}
		bg, ok := p.(batchGetter)
		if !ok || len(group) < 2 {
			singles = append(singles, group...)
			continue
		}
		fctx, cancel := context.WithTimeout(ctx, r.opts.Timeout)
		got, err := bg.GetMany(fctx, group)
		cancel()
		if err != nil {
			// fall back to single fetches, so the error names one reference
			singles = append(singles, group...)
			continue
		}
		for _, ref := range group {
			if raw, ok := got[ref.Key()]; ok {
				r.store(ref.Key(), raw)
			} else {
				singles = append(singles, ref)
			}
		}
	}

	sem := make(chan struct{}, maxConcurrentFetches)
	errs := make([]error, len(singles))
	var wg sync.WaitGroup
	for i, ref := range singles {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, ref Ref) {
			defer wg.Done()
			defer func() { <-sem }()
			_, errs[i] = r.fetch(ctx, ref)
		}(i, ref)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// replace copies v with every reference swapped for its value. path is the
// key path of v.
func (r *Resolver) replace(ctx context.Context, v any, path []string) (any, error) {
	switch t := v.(type) {
	case string:
		return r.resolve(ctx, t, path)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			nv, err := r.replace(ctx, item, append(slices.Clone(path), k))
			if err != nil {
				return nil, err
			}
			out[k] = nv
		}
		return out, nil
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			nv, err := r.replace(ctx, item, append(slices.Clone(path), fmt.Sprint(k)))
			if err != nil {
				return nil, err
			}
			out[fmt.Sprint(k)] = nv
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			nv, err := r.replace(ctx, item, path)
			if err != nil {
				return nil, err
			}
			out[i] = nv
		}
		return out, nil
	default:
		return v, nil
	}
}

// resolveEmbedded replaces each `ref+...+` inside s with its scalar value.
func (r *Resolver) resolveEmbedded(ctx context.Context, s string, path []string) (string, error) {
	var firstErr error
	out := embeddedRefRe.ReplaceAllStringFunc(s, func(m string) string {
		if firstErr != nil {
			return m
		}
		ref, err := ParseRef(m)
		if err != nil {
			firstErr = err
			return m
		}
		v, err := r.value(ctx, ref)
		if err != nil {
			firstErr = err
			return m
		}
		switch v.(type) {
		case map[string]any, []any:
			firstErr = r.wrap(ref, fmt.Errorf("an embedded reference must point to a scalar value"))
			return m
		}
		r.rememberAt(path, v)
		return fmt.Sprint(v)
	})
	if firstErr != nil {
		return "", firstErr
	}
	return out, nil
}

// choose picks the provider instance for ref (see "Instance choice").
func (r *Resolver) choose(ref Ref) (string, ProviderConfig, error) {
	cfg := r.opts.Config
	if ref.Provider != "" {
		pc, ok := cfg[ref.Provider]
		if !ok {
			return "", pc, fmt.Errorf("secret provider %q is not in secret_providers", ref.Provider)
		}
		if pc.Kind != ref.Kind {
			return "", pc, fmt.Errorf("secret provider %q has type %q, not %q", ref.Provider, pc.Kind, ref.Kind)
		}
		return ref.Provider, pc, nil
	}
	names := cfg.ofKind(ref.Kind)
	switch {
	case len(names) == 1:
		return names[0], cfg[names[0]], nil
	case len(names) > 1:
		if pc, ok := cfg["default"]; ok && pc.Kind == ref.Kind {
			return "default", pc, nil
		}
		return "", ProviderConfig{}, fmt.Errorf("several secret providers of type %q (%s). Add ?provider=<name> to the reference", ref.Kind, strings.Join(names, ", "))
	}
	return "~" + ref.Kind, ProviderConfig{Name: "default", Kind: ref.Kind, Props: map[string]any{}}, nil
}

// instance returns the built provider for ref, building it once.
func (r *Resolver) instance(ref Ref) (Provider, error) {
	name, pc, err := r.choose(ref)
	if err != nil {
		return nil, err
	}
	// imu, not mu: a factory may call Redact, which takes mu
	r.imu.Lock()
	defer r.imu.Unlock()
	if p, ok := r.instances[name]; ok {
		return p, nil
	}
	f := factoryOf(pc.Kind)
	if f == nil {
		return nil, fmt.Errorf("unknown secret backend %q", pc.Kind)
	}
	pc.baseDir = r.opts.BaseDir
	pc.timeout = r.opts.Timeout
	pc.redact = r.Redact
	for _, k := range CredentialKeys {
		r.remember(pc.Get(k))
	}
	p, err := f(pc)
	if err != nil {
		return nil, err
	}
	r.instances[name] = p
	return p, nil
}

func (r *Resolver) cached(key string) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.cache[key]
	if !ok || (!e.expires.IsZero() && time.Now().After(e.expires)) {
		return nil, false
	}
	return e.raw, true
}

func (r *Resolver) store(key string, raw []byte) {
	e := cacheEntry{raw: raw}
	if r.opts.CacheTTL > 0 {
		e.expires = time.Now().Add(r.opts.CacheTTL)
	}
	r.mu.Lock()
	r.cache[key] = e
	r.mu.Unlock()
}

// rememberAt records the secret values in v, which is at path. Keys of a map
// extend the path.
func (r *Resolver) rememberAt(path []string, v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, item := range t {
			r.rememberAt(append(slices.Clone(path), k), item)
		}
	case map[any]any:
		for k, item := range t {
			r.rememberAt(append(slices.Clone(path), fmt.Sprint(k)), item)
		}
	case []any:
		for _, item := range t {
			r.rememberAt(path, item)
		}
	case string:
		if len(path) == 0 || r.opts.IsSecret == nil || r.opts.IsSecret(path) {
			r.remember(t)
		}
	}
}

// remember records a resolved value for redaction.
func (r *Resolver) remember(v string) {
	v = strings.TrimSpace(v)
	if len(v) < minRedactLen {
		return
	}
	r.mu.Lock()
	r.values[v] = struct{}{}
	r.mu.Unlock()
	if r.opts.OnValue != nil {
		r.opts.OnValue(v)
	}
}

// wrap names the reference and the provider. It never shows the value.
func (r *Resolver) wrap(ref Ref, err error) error {
	if _, ok := err.(*ResolveError); ok {
		return err
	}
	name := "default"
	if n, _, cerr := r.choose(ref); cerr == nil && !strings.HasPrefix(n, "~") {
		name = n
	}
	return &ResolveError{Ref: ref.Raw, Provider: name, Kind: ref.Kind, Err: fmt.Errorf("%s", r.Redact(err.Error()))}
}

// ResolveError is the error of one failed reference.
type ResolveError struct {
	Ref      string
	Provider string
	Kind     string
	Err      error
}

func (e *ResolveError) Error() string {
	return fmt.Sprintf("could not resolve secret reference %q (provider %s, %s): %s", e.Ref, e.Provider, e.Kind, e.Err)
}

func (e *ResolveError) Unwrap() error { return e.Err }

// HasRef is true when v holds any reference, deeply.
func HasRef(v any) bool {
	found := false
	walkStrings(v, func(s string) {
		if !found && ContainsRef(s) {
			found = true
		}
	})
	return found
}

// Refs returns the references in v, deeply. A reference that does not
// parse is left out.
func Refs(v any) (refs []Ref) {
	walkStrings(v, func(s string) {
		for _, raw := range refsIn(s) {
			if ref, err := ParseRef(raw); err == nil {
				refs = append(refs, ref)
			}
		}
	})
	return refs
}

// refsIn returns the whole or embedded references in s.
func refsIn(s string) []string {
	if IsRef(s) {
		return []string{s}
	}
	return embeddedRefRe.FindAllString(s, -1)
}

// walkStrings calls fn for each string in v, deeply.
func walkStrings(v any, fn func(string)) {
	switch t := v.(type) {
	case string:
		fn(t)
	case map[string]any:
		for _, item := range t {
			walkStrings(item, fn)
		}
	case map[any]any:
		for _, item := range t {
			walkStrings(item, fn)
		}
	case []any:
		for _, item := range t {
			walkStrings(item, fn)
		}
	}
}

// trimNewline removes one trailing \n or \r\n.
func trimNewline(b []byte) []byte {
	s := string(b)
	if strings.HasSuffix(s, "\r\n") {
		return []byte(s[:len(s)-2])
	}
	return []byte(strings.TrimSuffix(s, "\n"))
}
