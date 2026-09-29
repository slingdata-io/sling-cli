package secrets

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProvider serves values from a map and counts calls.
type fakeProvider struct {
	name    string
	values  map[string]string
	calls   atomic.Int32
	batches atomic.Int32
	delay   time.Duration
}

func (p *fakeProvider) Get(_ context.Context, ref Ref) ([]byte, error) {
	p.calls.Add(1)
	time.Sleep(p.delay)
	v, ok := p.values[ref.Path]
	if !ok {
		return nil, errors.New("secret not found")
	}
	return []byte(v), nil
}

// fakeBatchProvider adds GetMany.
type fakeBatchProvider struct{ *fakeProvider }

func (p *fakeBatchProvider) GetMany(_ context.Context, refs []Ref) (map[string][]byte, error) {
	p.batches.Add(1)
	out := map[string][]byte{}
	for _, ref := range refs {
		if v, ok := p.values[ref.Path]; ok {
			out[ref.Key()] = []byte(v)
		}
	}
	return out, nil
}

var (
	fakeMu        sync.Mutex
	fakeInstances = map[string]*fakeProvider{}
	fakeValues    = map[string]string{
		"pg":      `{"username":"etl","password":"s3cr3t-pass","host":"db.acme.internal","dbname":"analytics","port":5432}`,
		"token":   "tok-123456\n",
		"crlf":    "crlf-value\r\n",
		"api_key": "key-abcdef",
	}
)

func init() {
	Register("fake", func(cfg ProviderConfig) (Provider, error) {
		fakeMu.Lock()
		defer fakeMu.Unlock()
		p := &fakeProvider{name: cfg.Name, values: fakeValues}
		fakeInstances[cfg.Name] = p
		return p, nil
	}, "fake_alias")
	Register("fakebatch", func(cfg ProviderConfig) (Provider, error) {
		fakeMu.Lock()
		defer fakeMu.Unlock()
		p := &fakeProvider{name: cfg.Name, values: fakeValues}
		fakeInstances["batch:"+cfg.Name] = p
		return &fakeBatchProvider{p}, nil
	})
	Register("fakeslow", func(cfg ProviderConfig) (Provider, error) {
		fakeMu.Lock()
		defer fakeMu.Unlock()
		p := &fakeProvider{name: cfg.Name, values: fakeValues, delay: 50 * time.Millisecond}
		fakeInstances["slow:"+cfg.Name] = p
		return p, nil
	})
}

func fakeInstance(name string) *fakeProvider {
	fakeMu.Lock()
	defer fakeMu.Unlock()
	return fakeInstances[name]
}

func TestResolverResolve(t *testing.T) {
	var seen []string
	r := NewResolver(Options{OnValue: func(v string) { seen = append(seen, v) }})
	ctx := context.Background()

	v, err := r.Resolve(ctx, "ref+fake://token")
	require.NoError(t, err)
	assert.Equal(t, "tok-123456", v, "one trailing newline is trimmed")

	v, _ = r.Resolve(ctx, "ref+fake://crlf")
	assert.Equal(t, "crlf-value", v)

	v, _ = r.Resolve(ctx, "ref+fake://token?trim=false")
	assert.Equal(t, "tok-123456\n", v)

	v, _ = r.Resolve(ctx, "ref+fake://pg#/password")
	assert.Equal(t, "s3cr3t-pass", v)

	v, _ = r.Resolve(ctx, "not a ref")
	assert.Equal(t, "not a ref", v)

	v, err = r.Resolve(ctx, "postgres://etl:ref+fake://pg#/password+@host/db")
	require.NoError(t, err)
	assert.Equal(t, "postgres://etl:s3cr3t-pass@host/db", v)

	_, err = r.Resolve(ctx, "ref+fake://pg")
	require.NoError(t, err, "no pointer: the whole JSON text is a string")

	assert.Contains(t, seen, "tok-123456")
	assert.Contains(t, seen, "s3cr3t-pass")
	assert.Equal(t, "x *** y", r.Redact("x s3cr3t-pass y"))
}

func TestResolverPointerToObject(t *testing.T) {
	fakeValues["nested"] = `{"a":{"b":"c-value"}}`
	r := NewResolver(Options{})
	_, err := r.Resolve(context.Background(), "ref+fake://nested#/a")
	assert.ErrorContains(t, err, "object or a list")
}

func TestResolverErrorHasNoValue(t *testing.T) {
	r := NewResolver(Options{})
	_, err := r.Resolve(context.Background(), "ref+fake://pg#/missing")
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, `could not resolve secret reference "ref+fake://pg#/missing" (provider default, fake)`)
	assert.NotContains(t, msg, "s3cr3t-pass")

	_, err = r.Resolve(context.Background(), "ref+fake://nope")
	assert.ErrorContains(t, err, "secret not found")
	var re *ResolveError
	assert.True(t, errors.As(err, &re))
	assert.Equal(t, "fake", re.Kind)
}

func TestResolverCache(t *testing.T) {
	cfg := Config{"c1": {Name: "c1", Kind: "fake", Props: map[string]any{}}}
	r := NewResolver(Options{Config: cfg, CacheTTL: 30 * time.Millisecond})
	ctx := context.Background()

	_, err := r.Resolve(ctx, "ref+fake://pg#/username")
	require.NoError(t, err)
	_, err = r.Resolve(ctx, "ref+fake://pg#/password")
	require.NoError(t, err)
	p := fakeInstance("c1")
	assert.EqualValues(t, 1, p.calls.Load(), "two pointers of one secret share one fetch")

	time.Sleep(50 * time.Millisecond)
	_, _ = r.Resolve(ctx, "ref+fake://pg#/username")
	assert.EqualValues(t, 2, p.calls.Load(), "TTL expired")
}

func TestResolverSingleflight(t *testing.T) {
	cfg := Config{"s1": {Name: "s1", Kind: "fakeslow", Props: map[string]any{}}}
	r := NewResolver(Options{Config: cfg})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.Resolve(context.Background(), "ref+fakeslow://token")
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, fakeInstance("slow:s1").calls.Load())
}

func TestResolverResolveValueBatch(t *testing.T) {
	cfg := Config{"b1": {Name: "b1", Kind: "fakebatch", Props: map[string]any{}}}
	r := NewResolver(Options{Config: cfg})
	in := map[string]any{
		"user":     "ref+fakebatch://pg#/username",
		"password": "ref+fakebatch://pg#/password",
		"list":     []any{"ref+fakebatch://token", "plain"},
		"nested":   map[string]any{"key": "ref+fakebatch://api_key"},
		"port":     5432,
	}
	out, err := r.ResolveValue(context.Background(), in)
	require.NoError(t, err)
	m := out.(map[string]any)
	assert.Equal(t, "etl", m["user"])
	assert.Equal(t, "s3cr3t-pass", m["password"])
	assert.Equal(t, []any{"tok-123456", "plain"}, m["list"])
	assert.Equal(t, "key-abcdef", m["nested"].(map[string]any)["key"])
	assert.Equal(t, 5432, m["port"])
	assert.Equal(t, "ref+fakebatch://pg#/password", in["password"], "input is not changed")

	p := fakeInstance("batch:b1")
	assert.EqualValues(t, 1, p.batches.Load())
	assert.EqualValues(t, 0, p.calls.Load())
}

func TestResolverResolveEntryFrom(t *testing.T) {
	r := NewResolver(Options{KeyAliases: map[string]string{"username": "user", "dbname": "database"}})
	out, err := r.ResolveEntry(context.Background(), map[string]any{
		"type":     "postgres",
		"from":     "ref+fake://pg",
		"database": "override_db",
		"sslmode":  "require",
	})
	require.NoError(t, err)
	assert.Equal(t, "postgres", out["type"])
	assert.Equal(t, "etl", out["user"], "username alias")
	assert.Equal(t, "override_db", out["database"], "entry key wins over the secret")
	assert.Equal(t, "s3cr3t-pass", out["password"])
	assert.Equal(t, "db.acme.internal", out["host"])
	assert.EqualValues(t, 5432, out["port"])
	assert.Equal(t, "require", out["sslmode"])
	assert.NotContains(t, out, "from")
	assert.NotContains(t, out, "username")
	assert.NotContains(t, out, "dbname")
	assert.Equal(t, "***", r.Redact("db.acme.internal"), "from values are redacted, also non-secret keys")

	_, err = r.ResolveEntry(context.Background(), map[string]any{"type": "postgres", "from": "literal"})
	assert.ErrorContains(t, err, "`from` must be a secret reference")

	_, err = r.ResolveEntry(context.Background(), map[string]any{"type": "postgres", "from": "ref+fake://token"})
	assert.ErrorContains(t, err, "needs a JSON or YAML object")
}

func TestResolverInstanceChoice(t *testing.T) {
	ctx := context.Background()

	// rule 1: ?provider=
	cfg := Config{
		"a":       {Name: "a", Kind: "fake"},
		"b":       {Name: "b", Kind: "fake"},
		"other":   {Name: "other", Kind: "file"},
		"default": {Name: "default", Kind: "fake"},
	}
	r := NewResolver(Options{Config: cfg})
	name, _, err := r.choose(mustRef(t, "ref+fake://token?provider=b"))
	require.NoError(t, err)
	assert.Equal(t, "b", name)
	_, _, err = r.choose(mustRef(t, "ref+fake://token?provider=other"))
	assert.ErrorContains(t, err, `has type "file", not "fake"`)
	_, _, err = r.choose(mustRef(t, "ref+fake://token?provider=zzz"))
	assert.ErrorContains(t, err, "not in secret_providers")

	// rule 3: several of one kind, one named default
	name, _, err = r.choose(mustRef(t, "ref+fake://token"))
	require.NoError(t, err)
	assert.Equal(t, "default", name)

	// rule 3: several, no default
	delete(cfg, "default")
	r = NewResolver(Options{Config: cfg})
	_, err = r.Resolve(ctx, "ref+fake://token")
	assert.ErrorContains(t, err, `several secret providers of type "fake" (a, b). Add ?provider=<name>`)

	// rule 2: exactly one
	name, _, err = r.choose(mustRef(t, "ref+file://x"))
	require.NoError(t, err)
	assert.Equal(t, "other", name)

	// rule 4: none, zero-config instance
	name, pc, err := NewResolver(Options{}).choose(mustRef(t, "ref+fake://token"))
	require.NoError(t, err)
	assert.Equal(t, "~fake", name)
	assert.Equal(t, "default", pc.Name)
}

func TestResolverCredentialRedaction(t *testing.T) {
	cfg := Config{"c": {Name: "c", Kind: "fake", Props: map[string]any{"token": "provider-token-xyz"}}}
	var seen []string
	r := NewResolver(Options{Config: cfg, OnValue: func(v string) { seen = append(seen, v) }})
	_, err := r.Resolve(context.Background(), "ref+fake://token")
	require.NoError(t, err)
	assert.Contains(t, seen, "provider-token-xyz")
	assert.Equal(t, "***", r.Redact("provider-token-xyz"))
}

func TestFileAndExecProviders(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secrets.yaml"), []byte("pg:\n  password: file-pass\n"), 0600))
	r := NewResolver(Options{BaseDir: dir})
	ctx := context.Background()

	v, err := r.Resolve(ctx, "ref+file://./secrets.yaml#/pg/password")
	require.NoError(t, err)
	assert.Equal(t, "file-pass", v)

	v, err = r.Resolve(ctx, "ref+file://"+filepath.Join(dir, "secrets.yaml")+"#pg/password")
	require.NoError(t, err)
	assert.Equal(t, "file-pass", v)

	v, err = r.Resolve(ctx, "ref+exec://echo?arg=exec-value")
	require.NoError(t, err)
	assert.Equal(t, "exec-value", v)

	_, err = r.Resolve(ctx, "ref+exec://no-such-binary-xyz")
	assert.ErrorContains(t, err, `"no-such-binary-xyz" is not installed`)

	_, err = r.Resolve(ctx, "ref+exec://sh?arg=-c&arg=echo%20oops%20>%262%3B%20exit%203")
	assert.ErrorContains(t, err, "sh failed: oops")
}

func TestParseConfig(t *testing.T) {
	cfg, err := ParseConfig(map[string]map[string]any{
		"op":  {"type": "1password", "token": "abc"},
		"aws": {"TYPE": "fake_alias", "region": "eu-west-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, "op", cfg["op"].Kind)
	assert.Equal(t, "fake", cfg["aws"].Kind)
	assert.Equal(t, "eu-west-1", cfg["aws"].Get("region"))
	assert.NotContains(t, cfg["aws"].Props, "type")

	_, err = ParseConfig(map[string]map[string]any{"x": {"region": "a"}})
	assert.ErrorContains(t, err, `secret provider "x" has no type`)
	_, err = ParseConfig(map[string]map[string]any{"x": {"type": "nope"}})
	assert.ErrorContains(t, err, `unknown type "nope"`)
	_, err = ParseConfig(map[string]map[string]any{"x": {"type": "fake", "token": "op://a/b/c"}})
	assert.ErrorContains(t, err, "cannot be a secret reference")

	t.Setenv("SLING_TEST_FAKE_ENV", "from-env")
	assert.Equal(t, "from-env", cfg["op"].Get("missing", "NOPE_UNSET", "SLING_TEST_FAKE_ENV"))
	assert.Equal(t, "abc", cfg["op"].Get("token", "SLING_TEST_FAKE_ENV"))
}

func mustRef(t *testing.T, s string) Ref {
	t.Helper()
	r, err := ParseRef(s)
	require.NoError(t, err)
	return r
}

// loopProvider resolves the reference in its path: ref+fakeloop://<ref>.
type loopProvider struct{ r *Resolver }

func (p loopProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	v, err := p.r.Resolve(ctx, "ref+fakeloop://"+ref.Path)
	if err != nil {
		return nil, err
	}
	return []byte(v.(string)), nil
}

func TestResolverCircularReference(t *testing.T) {
	var r *Resolver
	Register("fakeloop", func(ProviderConfig) (Provider, error) { return loopProvider{r: r}, nil })
	r = NewResolver(Options{})

	done := make(chan error, 1)
	go func() {
		_, err := r.Resolve(context.Background(), "ref+fakeloop://a")
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "circular reference: ref+fakeloop://a -> ref+fakeloop://a")
	case <-time.After(5 * time.Second):
		t.Fatal("resolve of a circular reference did not return")
	}
}
