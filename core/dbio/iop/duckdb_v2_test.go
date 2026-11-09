package iop

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckDbCliArgs(t *testing.T) {
	newDuck := func(version int, props ...string) *DuckDb {
		duck := NewDuckDb(context.Background(), props...)
		duck.version = version
		return duck
	}

	assert.NotContains(t, newDuck(10505).cliArgs(), "-no-agent")
	assert.Contains(t, newDuck(20000).cliArgs(), "-no-agent")

	args := newDuck(20000, "storage_version=v1.5.6").cliArgs()
	assert.Contains(t, args, "-storage-version")
	assert.Contains(t, args, "v1.5.6")

	assert.NotContains(t, newDuck(10505, "storage_version=v1.5.6").cliArgs(), "-storage-version")
	assert.Contains(t, newDuck(10505, "read_only=true").cliArgs(), "-readonly")
	assert.Contains(t, newDuck(20000, "read_only=true").cliArgs(), "-readonly")
}

func TestDuckDbAgentEnv(t *testing.T) {
	t.Setenv("AI_AGENT", "sling-test")
	t.Setenv("CLAUDECODE", "1")

	duck := NewDuckDb(context.Background())
	defer duck.Close()

	// total_changes counts for the session, so the insert runs first
	_, err := duck.Exec("create table t (i int)")
	require.NoError(t, err)
	res, err := duck.Exec("insert into t select range from range(3)")
	require.NoError(t, err)
	n, _ := res.RowsAffected()
	assert.EqualValues(t, 3, n)

	data, err := duck.Query("select 1 as a, 'x' as b")
	require.NoError(t, err)
	require.Len(t, data.Rows, 1)
	assert.Equal(t, int64(1), cast.ToInt64(data.Rows[0][0]))
	assert.Equal(t, "x", cast.ToString(data.Rows[0][1]))
}

func TestDuckDbChangesParse(t *testing.T) {
	duck := NewDuckDb(context.Background())
	defer duck.Close()

	_, err := duck.Exec("create table t (i int)")
	require.NoError(t, err)
	res, err := duck.Exec("insert into t values (1), (2), (3)")
	require.NoError(t, err)
	n, _ := res.RowsAffected()
	assert.EqualValues(t, 3, n)
}

func TestDuckDbLogWarnings(t *testing.T) {
	duck := NewDuckDb(context.Background())
	defer duck.Close()

	// the shell log storage of 2.0 prints log lines to stdout
	_, err := duck.Exec("set logging_level = 'DEBUG'")
	require.NoError(t, err)

	_, err = duck.Exec("create table t as select range as i from range(5)")
	require.NoError(t, err)

	data, err := duck.Query("select count(*) as cnt from t")
	require.NoError(t, err)
	require.Len(t, data.Rows, 1)
	assert.Equal(t, int64(5), cast.ToInt64(data.Rows[0][0]))
}

func TestDuckDbStorageVersion(t *testing.T) {
	makeFile := func(props ...string) string {
		dbPath := filepath.Join(t.TempDir(), "s.duckdb")
		duck := NewDuckDb(context.Background(), append(props, "instance="+dbPath)...)
		_, err := duck.Exec("create table t as select 1 as a")
		require.NoError(t, err)
		require.NoError(t, duck.Close())
		return dbPath
	}

	readOld := func(dbPath string) error {
		duck := NewDuckDb(context.Background(), "instance="+dbPath, "duckdb_version=1.5.5")
		defer duck.Close()
		_, err := duck.Query("select * from t")
		return err
	}

	assert.NoError(t, readOld(makeFile("storage_version=v1.5.6")))

	err := readOld(makeFile())
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "created with an newer version of DuckDB")
	}
}

func TestEnsureBinDuckDBPathVersion(t *testing.T) {
	bin, err := NewDuckDb(context.Background()).EnsureBinDuckDB("2.0.0")
	require.NoError(t, err)

	t.Setenv("DUCKDB_PATH", bin)
	duck := NewDuckDb(context.Background())
	_, err = duck.EnsureBinDuckDB("1.5.5")
	require.NoError(t, err)
	assert.Equal(t, 20000, duck.version)
}

func TestDuckDbSessionSQLReopen(t *testing.T) {
	duck := NewDuckDb(context.Background())
	defer duck.Close()

	duck.AddSessionSQL("ATTACH ':memory:' AS x")
	_, err := duck.Exec("create table x.t as select 1 as a")
	require.NoError(t, err)

	duck.kill() // the next query opens a new sidecar
	data, err := duck.Query("select count(*) as cnt from duckdb_databases() where database_name = 'x'")
	require.NoError(t, err)
	require.Len(t, data.Rows, 1)
	assert.Equal(t, int64(1), cast.ToInt64(data.Rows[0][0]), "the session SQL must attach x again")
}

func TestDuckDbArrowCastVariant(t *testing.T) {
	assert.Equal(t, "to_json(%s)::VARCHAR", duckArrowCast("VARIANT"))
	assert.Equal(t, "CAST(%s AS VARCHAR)", duckArrowCast(`"NULL"`))
	assert.Equal(t, "CAST(%s AS VARCHAR)", duckArrowCast("NULL"))

	duck := NewDuckDb(context.Background())
	defer duck.Close()

	_, err := duck.Exec(`create table t as select 1 as id, '{"a": 1}'::JSON::VARIANT as v, NULL as n`)
	require.NoError(t, err)

	ds, err := duck.StreamContext(context.Background(), "select * from t")
	require.NoError(t, err)
	data, err := ds.Collect(0)
	require.NoError(t, err)
	require.Len(t, data.Rows, 1)
	assert.JSONEq(t, `{"a": 1}`, cast.ToString(data.Rows[0][1]))
	assert.Nil(t, data.Rows[0][2])
}

func TestDuckDbQuackURI(t *testing.T) {
	cases := map[string]string{
		"quack:h":          "quack:h:9494",
		"quack:h:9000":     "quack:h:9000",
		"quack:[::1]":      "quack:[::1]:9494",
		"quack:[::1]:9000": "quack:[::1]:9000",
		"/tmp/x.db":        "",
		"":                 "",
	}
	for instance, want := range cases {
		duck := NewDuckDb(context.Background(), "instance="+instance)
		assert.Equal(t, want, duck.QuackURI(), instance)
	}
}

func TestDuckDbQuackAttachSQL(t *testing.T) {
	attach := func(props ...string) string {
		duck := NewDuckDb(context.Background(), append([]string{"instance=quack:h", "token=tok'1"}, props...)...)
		return duck.quackAttachSQL()
	}

	assert.Equal(t,
		"ATTACH IF NOT EXISTS 'quack:h:9494' AS \"quack\" (TYPE quack, SECRET sling_quack_secret);\nUSE \"quack\".\"main\"",
		attach())
	assert.Contains(t, attach("disable_ssl=true"), "DISABLE_SSL true")
	assert.Contains(t, attach("ssl_fingerprint=ab:cd"), "SSL_FINGERPRINT 'ab:cd'")
	assert.Contains(t, attach("client_id=sl'ing"), "CLIENT_ID 'sl''ing'")
	assert.Contains(t, attach("heartbeat_timeout=30"), "HEARTBEAT_TIMEOUT 30")
	assert.Contains(t, attach("read_only=true"), "READ_ONLY")
	assert.Contains(t, attach("database=rem", "schema=stg"), `AS "rem" (`)
	assert.Contains(t, attach("database=rem", "schema=stg"), `USE "rem"."stg"`)
	assert.NotContains(t, attach(), "tok")

	duck := NewDuckDb(context.Background(), "instance=quack:h", "token=tok'1", `extra_http_headers={"X-B": "2", "X-A": "1"}`)
	quackSecret := duck.quackSecret()
	secret := quackSecret.Render()
	assert.Equal(t,
		"CREATE SECRET IF NOT EXISTS sling_quack_secret (TYPE quack, EXTRA_HTTP_HEADERS MAP{'X-A': '1', 'X-B': '2'}, SCOPE 'quack:h:9494', TOKEN 'tok''1')",
		secret)
}

func TestDuckDbSecretRenderRaw(t *testing.T) {
	secret := NewDuckDbSecret("s", DuckDbSecretTypeS3, map[string]string{
		"region":             "us-east-1",
		"provider":           "credential_chain",
		"extra_http_headers": "MAP{'a': 'b'}",
		"key_id":             "k",
	})
	for range 5 {
		assert.Equal(t,
			"CREATE SECRET IF NOT EXISTS s (TYPE s3, EXTRA_HTTP_HEADERS MAP{'a': 'b'}, KEY_ID 'k', PROVIDER credential_chain, REGION 'us-east-1')",
			secret.Render())
	}
}

func TestDuckMapLiteral(t *testing.T) {
	assert.Equal(t, "MAP{}", DuckMapLiteral(nil))
	assert.Equal(t, "MAP{'a': '1', 'b''x': 'it''s'}", DuckMapLiteral(map[string]string{"b'x": "it's", "a": "1"}))
}

func TestDuckDbCliArgsQuack(t *testing.T) {
	duck := NewDuckDb(context.Background(), "instance=quack:h", "read_only=true")
	duck.version = 20000
	args := duck.cliArgs()
	assert.NotContains(t, args, "-readonly")
	assert.NotContains(t, args, "quack:h")
}
