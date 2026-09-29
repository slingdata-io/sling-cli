package connection

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRejectLiteralSecretsNested(t *testing.T) {
	err := RejectLiteralSecrets("MY_API", map[string]any{
		"type": "api",
		"spec": "stripe",
		"secrets": map[string]any{
			"api_key": "sk_live_distinctive",
		},
	})
	if err == nil {
		t.Fatal("expected literal nested secret to be refused")
	}
	if !strings.Contains(err.Error(), "secrets.api_key") && !strings.Contains(err.Error(), "api_key") {
		t.Fatalf("error should name the field: %v", err)
	}
}

func TestSecretRefConnection(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "secret.db")
	secretFile := filepath.Join(dir, "instance.txt")
	require.NoError(t, os.WriteFile(secretFile, []byte(dbPath+"\n"), 0600))
	ref := "ref+file://" + secretFile

	conn, err := NewConnection("SECRET_SQLITE", "sqlite", g.M("instance", ref))
	require.NoError(t, err)
	hash := conn.Hash()
	assert.True(t, conn.HasSecretRef())

	rc, err := conn.Resolved(context.Background())
	require.NoError(t, err)
	assert.Equal(t, dbPath, rc.Data["instance"])
	assert.Contains(t, rc.URL(), dbPath)
	assert.NotContains(t, rc.URL(), "ref+file")

	dc, err := conn.AsDatabaseContext(context.Background(), AsConnOptions{})
	require.NoError(t, err)
	require.NoError(t, dc.Connect())
	defer conn.Close()
	_, err = dc.Exec("create table t (a int)")
	require.NoError(t, err)
	assert.FileExists(t, dbPath)

	assert.Equal(t, ref, conn.Data["instance"], "data keeps the reference")
	assert.Equal(t, hash, conn.Hash(), "hash is stable")

	plain, _ := NewConnection("PLAIN", "sqlite", g.M("instance", dbPath))
	rp, err := plain.Resolved(context.Background())
	require.NoError(t, err)
	assert.Same(t, &plain, rp)
}

func TestSecretRefDerivedURL(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "pg.json")
	require.NoError(t, os.WriteFile(secretFile, []byte(`{"password":"p@ss word","username":"etl","dbname":"app"}`), 0600))
	ref := "ref+file://" + secretFile + "#/password"

	conn, err := NewConnection("SECRET_PG", "postgres", g.M("host", "h", "user", "u", "database", "d", "password", ref))
	require.NoError(t, err)
	assert.True(t, conn.derivedURL())

	rc, err := conn.Resolved(context.Background())
	require.NoError(t, err)
	assert.Contains(t, rc.URL(), "p%40ss%20word")
	assert.NotContains(t, rc.URL(), "ref+file")
	assert.Contains(t, conn.URL(), "ref%2Bfile", "original URL keeps the reference")

	// from: merge, key aliases, explicit keys win
	from, err := NewConnectionFromMap(g.M("name", "FROM_PG", "type", "postgres", "data", g.M("from", "ref+file://"+secretFile, "host", "h", "database", "override")))
	require.NoError(t, err)
	rc, err = from.Resolved(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "etl", rc.Data["user"])
	assert.Equal(t, "override", rc.Data["database"])
	assert.Equal(t, "p@ss word", rc.Data["password"])

}

func TestSecretRefLazyType(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(content), 0600))
		return "ref+file://" + path
	}
	rds := write("rds.json", `{"engine":"postgres","host":"h","username":"u","password":"p","dbname":"d"}`)
	typed := write("typed.json", `{"type":"mysql","host":"h","user":"u","password":"p","database":"d"}`)
	byURL := write("url.json", `{"url":"sqlserver://u:p@h:1433/d"}`)
	none := write("none.json", `{"host":"h"}`)

	for ref, want := range map[string]dbio.Type{rds: dbio.TypeDbPostgres, typed: dbio.TypeDbMySQL, byURL: dbio.TypeDbSQLServer} {
		conn, err := NewConnectionFromMap(g.M("name", "LAZY", "data", g.M("from", ref)))
		require.NoError(t, err)
		assert.True(t, conn.Type.IsUnknown(), "no secret read at load")
		require.NoError(t, conn.ResolveType(context.Background()))
		assert.Equal(t, want, conn.Type)
	}

	// an explicit type wins over the secret
	conn, err := NewConnectionFromMap(g.M("name", "LAZY", "type", "redshift", "data", g.M("from", rds)))
	require.NoError(t, err)
	require.NoError(t, conn.ResolveType(context.Background()))
	assert.Equal(t, dbio.TypeDbRedshift, conn.Type)

	conn, err = NewConnectionFromMap(g.M("name", "LAZY_NONE", "data", g.M("from", none)))
	require.NoError(t, err)
	assert.ErrorContains(t, conn.ResolveType(context.Background()), "could not find the type of connection LAZY_NONE")
	_, err = conn.AsDatabase()
	assert.ErrorContains(t, err, "could not find the type")
}

func TestSecretRefErrors(t *testing.T) {
	conn, err := NewConnection("MISSING", "sqlite", g.M("instance", "ref+file:///no/such/file.txt"))
	require.NoError(t, err)
	_, err = conn.AsDatabase(AsConnOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `could not resolve secret reference "ref+file:///no/such/file.txt"`)
}

func TestRejectLiteralSecretsAcceptsRefs(t *testing.T) {
	err := RejectLiteralSecrets("MY_PG", map[string]any{
		"type":     "postgres",
		"password": "op://Data/pg/password",
		"secrets":  map[string]any{"api_key": "ref+vault://secret/data/x#/key"},
	})
	assert.NoError(t, err)

	err = RejectLiteralSecrets("MY_PG", map[string]any{"type": "postgres", "password": "literal"})
	assert.ErrorContains(t, err, "or a secret reference such as op://vault/item/field")
}

func TestSecretRefEmbeddedURL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "embedded.db")
	secretFile := filepath.Join(dir, "path.txt")
	require.NoError(t, os.WriteFile(secretFile, []byte(dbPath), 0600))

	conn, err := NewConnectionFromURL("EMBEDDED", "sqlite://ref+file://"+secretFile+"+")
	require.NoError(t, err)
	assert.EqualValues(t, "sqlite", conn.Type)

	rc, err := conn.Resolved(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "sqlite://"+dbPath, rc.Data["url"])
	assert.Equal(t, dbPath, rc.Data["instance"])
}

func TestSecretRefSlingProvider(t *testing.T) {
	t.Setenv("ENV_YAML", `
connections:
  SLING_SRC_PG:
    type: postgres
    host: h1
    user: u1
    database: d1
    password: ref+exec://echo?arg=pw-sling-1
    options:
      nested:
        key: deep-value
  SLING_COPY_PG:
    type: postgres
    from: ref+sling://connections/sling_src_pg
    database: other
  SLING_SELF:
    type: postgres
    host: h
    user: u
    database: d
    password: ref+sling://connections/sling_self/host
`)
	t.Setenv("SLING_T_KEY", "ref+exec://echo?arg=env-val-1")
	t.Setenv("SLING_T_LOOP", "ref+sling://env/SLING_T_LOOP")
	GetLocalConns(true)

	r, err := env.SecretResolver()
	require.NoError(t, err)
	ctx := context.Background()
	get := func(ref string) (any, error) { return r.Resolve(ctx, ref) }

	v, err := get("ref+sling://connections/sling_src_pg/host")
	require.NoError(t, err)
	assert.Equal(t, "h1", v)

	v, err = get("ref+sling://connections/SLING_SRC_PG/password")
	require.NoError(t, err)
	assert.Equal(t, "pw-sling-1", v, "refs in the source connection resolve")

	v, err = get("ref+sling://connections/sling_src_pg/options/nested/key")
	require.NoError(t, err)
	assert.Equal(t, "deep-value", v)

	v, err = get("ref+sling://connections/sling_src_pg/url")
	require.NoError(t, err)
	assert.Contains(t, v, "pw-sling-1")

	v, err = get("ref+sling://env/SLING_T_KEY")
	require.NoError(t, err)
	assert.Equal(t, "env-val-1", v)

	// from: copies a connection, keys written in the entry win
	copyConn := GetLocalConns().Get("SLING_COPY_PG").Connection
	rc, err := copyConn.Resolved(ctx)
	require.NoError(t, err)
	assert.Equal(t, "h1", rc.Data["host"])
	assert.Equal(t, "pw-sling-1", rc.Data["password"])
	assert.Equal(t, "other", rc.Data["database"])

	_, err = get("ref+sling://connections/sling_self/password")
	assert.ErrorContains(t, err, "circular reference")
	_, err = get("ref+sling://env/SLING_T_LOOP")
	assert.ErrorContains(t, err, "circular reference")
	_, err = get("ref+sling://connections/sling_src_pg/nosuch")
	assert.ErrorContains(t, err, `has no field "nosuch"`)
	_, err = get("ref+sling://connections/no_such_conn/host")
	assert.ErrorContains(t, err, "did not find connection no_such_conn")
	_, err = get("ref+sling://other/x")
	assert.ErrorContains(t, err, "unknown sling scope")
	_, err = get("ref+sling://env/SLING_T_UNSET_KEY")
	assert.ErrorContains(t, err, "SLING_T_UNSET_KEY is not set")
}

func TestSecretRefTestFailsOnResolveError(t *testing.T) {
	conn, err := NewConnection("SECRET_MISSING", "", g.M("from", "ref+file://"+filepath.Join(t.TempDir(), "missing.json")))
	require.NoError(t, err)
	assert.True(t, conn.Type.IsUnknown())

	ok, err := conn.Test()
	assert.False(t, ok)
	assert.ErrorContains(t, err, "could not resolve secret reference")
}
