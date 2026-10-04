package secrets

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsRef(t *testing.T) {
	cases := map[string]bool{
		"op://Data/MotherDuck/token":                 true,
		"op://My Vault/item/field":                   true,
		"keeper://UID/field/password":                true,
		"ref+awssecrets://prod/postgres#/password":   true,
		"ref+vault://secret/data/stripe#/api_key+":   true,
		"  ref+file://./secrets.yaml  ":              true,
		"ref+exec://tool?arg=a&arg=b":                true,
		"${PG_PASSWORD}":                             false,
		"postgres://user:pass@host/db":               false,
		"postgres://etl:ref+op://Data/pg/pass+@host": false,
		"ref+op://a+b":                               false,
		"ref+://x":                                   false,
		"ref+Op://x":                                 false,
		"op://":                                      false,
		"plain secret":                               false,
		"":                                           false,
		"op://a\nb":                                  false,
	}
	for s, want := range cases {
		assert.Equal(t, want, IsRef(s), s)
	}
	assert.True(t, ContainsRef("postgres://etl:ref+op://Data/pg/pass+@host/db"))
	assert.False(t, ContainsRef("postgres://etl:secret@host/db"))
}

func TestParseRef(t *testing.T) {
	type want struct {
		kind, path, pointer, provider string
		params                        map[string]string
		trim                          bool
	}
	cases := map[string]want{
		"op://Data/MotherDuck/token":                   {kind: "op", path: "Data/MotherDuck/token", trim: true},
		"op://Prod/PG/creds/password?attribute=otp":    {kind: "op", path: "Prod/PG/creds/password", params: map[string]string{"attribute": "otp"}, trim: true},
		"ref+1password://Data/x/y":                     {kind: "op", path: "Data/x/y", trim: true},
		"ref+awssecrets://prod/postgres#/password":     {kind: "awssecrets", path: "prod/postgres", pointer: "/password", trim: true},
		"ref+awssecrets://prod/postgres#password":      {kind: "awssecrets", path: "prod/postgres", pointer: "/password", trim: true},
		"ref+awsssm:///prod/pg/password?region=eu":     {kind: "awsssm", path: "/prod/pg/password", params: map[string]string{"region": "eu"}, trim: true},
		"ref+vault://secret/stripe?provider=v2#/a/b":   {kind: "vault", path: "secret/stripe", pointer: "/a/b", provider: "v2", trim: true},
		"ref+file://./x.txt?trim=false":                {kind: "file", path: "./x.txt", trim: false},
		"ref+file://./x.txt+":                          {kind: "file", path: "./x.txt", trim: true},
		"ref+aws_secrets_manager://arn:aws:sm:x#/user": {kind: "awssecrets", path: "arn:aws:sm:x", pointer: "/user", trim: true},
	}
	for s, w := range cases {
		ref, err := ParseRef(s)
		require.NoError(t, err, s)
		assert.Equal(t, w.kind, ref.Kind, s)
		assert.Equal(t, w.path, ref.Path, s)
		assert.Equal(t, w.pointer, ref.Pointer, s)
		assert.Equal(t, w.provider, ref.Provider, s)
		assert.Equal(t, w.trim, ref.Trim, s)
		assert.Equal(t, len(w.params), len(ref.Params), s)
		for k, v := range w.params {
			assert.Equal(t, v, ref.Params.Get(k), s)
		}
		assert.Equal(t, s, ref.Raw)
	}

	_, err := ParseRef("ref+nosuch://x")
	assert.ErrorContains(t, err, `unknown secret backend "nosuch"`)
	_, err = ParseRef("not a ref")
	assert.Error(t, err)
}

func TestRefKey(t *testing.T) {
	a, _ := ParseRef("ref+awssecrets://prod/pg?version_stage=AWSCURRENT#/user")
	b, _ := ParseRef("ref+awssecrets://prod/pg?version_stage=AWSCURRENT#/password")
	c, _ := ParseRef("ref+awssecrets://prod/pg?provider=eu#/password")
	assert.Equal(t, a.Key(), b.Key(), "pointer is not part of the key")
	assert.NotEqual(t, a.Key(), c.Key(), "provider is part of the key")
}

func TestRefSelect(t *testing.T) {
	ref := func(s string) Ref {
		r, err := ParseRef(s)
		require.NoError(t, err)
		return r
	}

	v, err := ref("ref+file://x").Select([]byte("plain"))
	require.NoError(t, err)
	assert.Equal(t, "plain", v)

	jsonDoc := []byte(`{"pg":{"password":"p@ss","port":5432,"hosts":["a","b"]},"a/b":"slash","t~x":"tilde"}`)
	v, err = ref("ref+file://x#/pg/password").Select(jsonDoc)
	require.NoError(t, err)
	assert.Equal(t, "p@ss", v)
	v, _ = ref("ref+file://x#/pg/port").Select(jsonDoc)
	assert.EqualValues(t, 5432, v)
	v, _ = ref("ref+file://x#/pg/hosts/1").Select(jsonDoc)
	assert.Equal(t, "b", v)
	v, _ = ref("ref+file://x#/a~1b").Select(jsonDoc)
	assert.Equal(t, "slash", v)
	v, _ = ref("ref+file://x#/t~0x").Select(jsonDoc)
	assert.Equal(t, "tilde", v)
	v, _ = ref("ref+file://x#/pg").Select(jsonDoc)
	assert.IsType(t, map[string]any{}, v)

	yamlDoc := []byte("pg:\n  password: fromyaml\n  1: one\n")
	v, err = ref("ref+file://x#/pg/password").Select(yamlDoc)
	require.NoError(t, err)
	assert.Equal(t, "fromyaml", v)
	v, _ = ref("ref+file://x#/pg/1").Select(yamlDoc)
	assert.Equal(t, "one", v)

	_, err = ref("ref+file://x#/pg/missing").Select(jsonDoc)
	assert.ErrorContains(t, err, `key "missing" not found`)
	assert.NotContains(t, err.Error(), "p@ss")
	_, err = ref("ref+file://x#/pg/password/deeper").Select(jsonDoc)
	assert.ErrorContains(t, err, "past a scalar")
	_, err = ref("ref+file://x#/a").Select([]byte("just: [unclosed"))
	assert.ErrorContains(t, err, "not JSON or YAML")
}

func TestNativeRef(t *testing.T) {
	for raw, want := range map[string]Ref{
		"https://my-vault.vault.azure.net/secrets/pg-password":                                     {Kind: "azurekeyvault", Path: "https://my-vault.vault.azure.net/secrets/pg-password"},
		"https://My-Vault.vault.usgovcloudapi.net:443/secrets/pg/ec96f02080254f109c51a1f14cdb1931": {Kind: "azurekeyvault", Path: "https://My-Vault.vault.usgovcloudapi.net:443/secrets/pg/ec96f02080254f109c51a1f14cdb1931"},
		"arn:aws:secretsmanager:eu-west-2:123456789012:secret:prod/pg-a1b2c3#/password":            {Kind: "awssecrets", Path: "arn:aws:secretsmanager:eu-west-2:123456789012:secret:prod/pg-a1b2c3", Pointer: "/password"},
		"arn:aws-us-gov:ssm:us-gov-west-1:123456789012:parameter/prod/pg/password":                 {Kind: "awsssm", Path: "arn:aws-us-gov:ssm:us-gov-west-1:123456789012:parameter/prod/pg/password"},
		"projects/acme/secrets/pg/versions/5":                                                      {Kind: "gcpsecrets", Path: "projects/acme/secrets/pg/versions/5"},
		"projects/acme/locations/us-central1/secrets/pg":                                           {Kind: "gcpsecrets", Path: "projects/acme/locations/us-central1/secrets/pg"},
		"//secretmanager.googleapis.com/projects/acme/secrets/pg":                                  {Kind: "gcpsecrets", Path: "//secretmanager.googleapis.com/projects/acme/secrets/pg"},
		"myorg:variable:prod/db/password":                                                          {Kind: "conjur", Path: "myorg:variable:prod/db/password"},
	} {
		if !IsRef(raw) {
			t.Fatalf("IsRef(%q) is false", raw)
		}
		ref, err := ParseRef(raw)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Kind != want.Kind || ref.Path != want.Path || ref.Pointer != want.Pointer {
			t.Fatalf("%s: got kind %q path %q pointer %q", raw, ref.Kind, ref.Path, ref.Pointer)
		}
	}

	for _, raw := range []string{
		"https://my-vault.vault.azure.net/keys/k1",
		"https://example.com/secrets/pg",
		"https://my-vault.managedhsm.azure.net/secrets/pg",
		"arn:aws:iam::123456789012:role/sling",
		"arn:aws:s3:::my-bucket",
		"projects/acme/datasets/sales",
		"gs://bucket/projects/acme/secrets/pg",
		"select 1 as variable",
	} {
		if IsRef(raw) {
			t.Fatalf("IsRef(%q) is true", raw)
		}
	}
}

func TestRefs(t *testing.T) {
	v := map[string]any{
		"a": "op://vault/item/field",
		"b": []any{"plain", `{secret("ref+exec://ls")}`},
		"c": map[string]any{"d": "ref+file:///etc/hosts"},
	}
	got := []string{}
	for _, ref := range Refs(v) {
		got = append(got, ref.Kind+":"+ref.Path)
	}
	assert.ElementsMatch(t, []string{"op:vault/item/field", `exec:ls")}`, "file:/etc/hosts"}, got)
}
